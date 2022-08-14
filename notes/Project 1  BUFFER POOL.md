# Project 1 : BUFFER POOL

在本实验中，需要在存储管理器中实现缓冲池。缓冲池负责将物理页面从磁盘中读入内存、或从内存中写回磁盘，使得DBMS可以支持大于内存大小的存储容量。并且，缓冲池应当是用户透明且线程安全的。

## Task1 : LRU REPLACEMENT POLICY

 本部分中需要实现缓冲池中的`LRUReplacer`，该组件的功能是跟踪缓冲池内的页面使用情况，并在缓冲池容量不足时驱除缓冲池中最近最少使用的页面。其应当具备如下接口：

- `Victim(frame_id_t*)`：驱逐缓冲池中最近最少使用的页面，并将其内容存储在输入参数中。当`LRUReplacer`为空时返回False，否则返回True；
- `Pin(frame_id_t)`：当缓冲池中的页面被用户访问时，该方法被调用使得该页面从`LRUReplacer`中驱逐，以使得该页面固定在缓存池中；
- `Unpin(frame_id_t)`：当缓冲池的页面被所有用户使用完毕时，该方法被调用使得该页面被添加在`LRUReplacer`，使得该页面可以被缓冲池驱逐；
- `Size()`：返回`LRUReplacer`中页面的数目；

```c++
 28 class LRUReplacer : public Replacer {
 29  public:
 30   /**
 31    * Create a new LRUReplacer.
 32    * @param num_pages the maximum number of pages the LRUReplacer will be required to store
 33    */
 34   explicit LRUReplacer(size_t num_pages);
 35   
 36   /**
 37    * Destroys the LRUReplacer.
 38    */
 39   ~LRUReplacer() override;
 40   
 41   bool Victim(frame_id_t *frame_id) override;
 42 
 43   void Pin(frame_id_t frame_id) override;
 44   
 45   void Unpin(frame_id_t frame_id) override;
 46   
 47   size_t Size() override;
 48 
 49   void DeleteNode(LinkListNode *curr);
 50 
 51  private:
 52   // TODO(student): implement me!
 53   std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> data_idx_;
 54   std::list<frame_id_t> data_;
 55   std::mutex data_latch_;
 56 };
```

在这里，LRU策略可以由**哈希表加双向链表**的方式实现，其中链表充当队列的功能以记录页面被访问的先后顺序，哈希表则记录<页面ID - 链表节点>键值对，以在O(1)复杂度下删除链表元素。实际实现中使用STL中的哈希表`unordered_map`和双向链表`list`，并在`unordered_map`中存储指向链表节点的`list::iterator`。

```C++
 21 bool LRUReplacer::Victim(frame_id_t *frame_id) {
 22   data_latch_.lock();
 23   if (data_idx_.empty()) {
 24     data_latch_.unlock();
 25     return false;
 26   }
 27   *frame_id = data_.front();
 28   data_.pop_front();
 29   data_idx_.erase(*frame_id);
 30   data_latch_.unlock();
 31   return true;
 32 }
```

对于`Victim`，首先判断链表是否为空，如不为空则返回链表首节点的页面ID，并在哈希表中解除指向首节点的映射。为了保证线程安全，整个函数应当由`mutex`互斥锁保护，下文中对互斥锁操作不再赘述。

```C++
 34 void LRUReplacer::Pin(frame_id_t frame_id) {
 35   data_latch_.lock();
 36   auto it = data_idx_.find(frame_id);
 37   if (it != data_idx_.end()) {
 38     data_.erase(it->second);
 39     data_idx_.erase(it);
 40   }
 41   data_latch_.unlock();
 42 }
```

对于`Pin`，其检查`LRUReplace`中是否存在对应页面ID的节点，如不存在则直接返回，如存在对应节点则通过哈希表中存储的迭代器删除链表节点，并解除哈希表对应页面ID的映射。

```C++
 44 void LRUReplacer::Unpin(frame_id_t frame_id) {
 45   data_latch_.lock();
 46   auto it = data_idx_.find(frame_id);
 47   if (it == data_idx_.end()) {
 48     data_.push_back(frame_id);
 49     data_idx_[frame_id] = prev(data_.end());
 50   }
 51   data_latch_.unlock();
 52 }
```

对于`Unpin`，其检查`LRUReplace`中是否存在对应页面ID的节点，如存在则直接返回，如不存在则在链表尾部插入页面ID的节点，并在哈希表中插入<页面ID - 链表尾节点>映射。

```C++
 54 size_t LRUReplacer::Size() {
 55   data_latch_.lock();
 56   size_t ret = data_idx_.size();
 57   data_latch_.unlock();
 58   return ret;
 59 }
```

对于`Size`，返回哈希表大小即可。

## Task2 : BUFFER POOL MANAGER INSTANCE

在部分中，需要实现缓冲池管理模块，其从`DiskManager`中获取数据库页面，并在缓冲池强制要求时或驱逐页面时将数据库脏页面写回`DiskManager`。

```C++
 30 class BufferPoolManagerInstance : public BufferPoolManager {
...
134   Page *pages_;
135   /** Pointer to the disk manager. */
136   DiskManager *disk_manager_ __attribute__((__unused__));
137   /** Pointer to the log manager. */
138   LogManager *log_manager_ __attribute__((__unused__));
139   /** Page table for keeping track of buffer pool pages. */
140   std::unordered_map<page_id_t, frame_id_t> page_table_;
141   /** Replacer to find unpinned pages for replacement. */
142   Replacer *replacer_;
143   /** List of free pages. */
144   std::list<frame_id_t> free_list_;
145   /** This latch protects shared data structures. We recommend updating this comment to describe     what it protects. */
146   std::mutex latch_;
147 };

```

缓冲池的成员如上所示，其中`pages_`为缓冲池中的实际容器页面槽位数组，用于存放从磁盘中读入的页面，并供DBMS访问；`disk_manager_`为磁盘管理器，提供从磁盘读入页面及写入页面的接口；·`log_manager_`为日志管理器，本实验中不用考虑该组件；`page_table_`用于保存磁盘页面ID`page_id`和槽位ID`frame_id_t`的映射；`raplacer_`用于选取所需驱逐的页面；`free_list_`保存缓冲池中的空闲槽位ID。在这里，区分`page_id`和`frame_id_t`是完成本实验的关键。

```C++
 28 class Page {
 29   // There is book-keeping information inside the page that should only be relevant to the buffer     pool manager.
 30   friend class BufferPoolManagerInstance;
 31 
 32  public:
 33   /** Constructor. Zeros out the page data. */
 34   Page() { ResetMemory(); }
 35 
 36   /** Default destructor. */
 37   ~Page() = default;
 38 
 39   /** @return the actual data contained within this page */
 40   inline auto GetData() -> char * { return data_; }
 41 
 42   /** @return the page id of this page */
 43   inline auto GetPageId() -> page_id_t { return page_id_; }
 44 
 45   /** @return the pin count of this page */
 46   inline auto GetPinCount() -> int { return pin_count_; }
 47 
 48   /** @return true if the page in memory has been modified from the page on disk, false otherwise     */
 49   inline auto IsDirty() -> bool { return is_dirty_; }
...
 77  private:
 78   /** Zeroes out the data that is held within the page. */
 79   inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }
 80 
 81   /** The actual data that is stored within a page. */
 82   char data_[PAGE_SIZE]{};
 83   /** The ID of this page. */
 84   page_id_t page_id_ = INVALID_PAGE_ID;
 85   /** The pin count of this page. */
 86   int pin_count_ = 0;
 87   /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
 88   bool is_dirty_ = false;
 89   /** Page latch. */
 90   ReaderWriterLatch rwlatch_;
 91 };

```

`Page`是缓冲池中的页面容器，`data_`保存对应磁盘页面的实际数据；`page_id_`保存该页面在磁盘管理器中的页面ID；`pin_count_`保存DBMS中正使用该页面的用户数目；`is_dirty_`保存该页面自磁盘读入或写回后是否被修改。下面，将介绍缓冲池中的接口实现：

```C++
 51 bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
 52   // Make sure you call DiskManager::WritePage!
 53   frame_id_t frame_id;
 54   latch_.lock();
 55   if (page_table_.count(page_id) == 0U) {
 56     latch_.unlock();
 57     return false;
 58   }
 59   frame_id = page_table_[page_id];
 60   pages_[frame_id].is_dirty_ = false;
 61   disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
 62   latch_.unlock();
 63   return true;
 64 }
```

`FlushPgImp`用于显式地将缓冲池页面写回磁盘。首先，应当检查缓冲池中是否存在对应页面ID的页面，如不存在则返回False；如存在对应页面，则将缓冲池内的该页面的`is_dirty_`置为false，并使用`WritePage`将该页面的实际数据`data_`写回磁盘。在这里，需要使用互斥锁保证线程安全，在下文中不再赘述。

```C++
 66 void BufferPoolManagerInstance::FlushAllPgsImp() {
 67   // You can do it!
 68   latch_.lock();
 69   for (auto [page_id, frame_id] : page_table_) {
 70     pages_[frame_id].is_dirty_ = false;
 71     disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
 72   }
 73   latch_.unlock();
 74 }
```

`FlushAllPgsImp`将缓冲池内的所有页面写回磁盘。在这里，遍历`page_table_`以获得缓冲池内的<页面ID - 槽位ID>对，通过槽位ID获取实际页面，并通过页面ID作为写回磁盘的参数。

```C++
 76 Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
 77   // 0.   Make sure you call AllocatePage!
 78   // 1.   If all the pages in the buffer pool are pinned, return nullptr.
 79   // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
 80   // 3.   Update P's metadata, zero out memory and add P to the page table.
 81   // 4.   Set the page ID output parameter. Return a pointer to P.
 82   frame_id_t new_frame_id;
 83   latch_.lock();
 84   if (!free_list_.empty()) {
 85     new_frame_id = free_list_.front();
 86     free_list_.pop_front();
 87   } else if (!replacer_->Victim(&new_frame_id)) {
 88     latch_.unlock();
 89     return nullptr;
 90   }
 91   *page_id = AllocatePage();
 92   if (pages_[new_frame_id].IsDirty()) {
 93     page_id_t flush_page_id = pages_[new_frame_id].page_id_;
 94     pages_[new_frame_id].is_dirty_ = false;
 95     disk_manager_->WritePage(flush_page_id, pages_[new_frame_id].GetData());
 96   }
 97   page_table_.erase(pages_[new_frame_id].page_id_);
 98   page_table_[*page_id] = new_frame_id;
 99   pages_[new_frame_id].page_id_ = *page_id;
100   pages_[new_frame_id].ResetMemory();
101   pages_[new_frame_id].pin_count_ = 1;
102   replacer_->Pin(new_frame_id);
103   latch_.unlock();
104   return &pages_[new_frame_id];
105 }
```

`NewPgImp`在磁盘中分配新的物理页面，将其添加至缓冲池，并返回指向缓冲池页面`Page`的指针。在这里，该函数由以下步骤组成：

1. 检查当前缓冲池中是否存在空闲槽位或存放页面可被驱逐的槽位（下文称其为目标槽位），在这里总是先通过检查`free_list_`以查询空闲槽位，如无空闲槽位则尝试从`replace_`中驱逐页面并返回被驱逐页面的槽位。如目标槽位，则返回空指针；如存在目标槽位，则调用`AllocatePage()`为新的物理页面分配`page_id`页面ID。
2. 值得注意的是，在这里需要检查目标槽位中的页面是否为脏页面，如是则需将其写回磁盘，并将其脏位设为false；
3. 从`page_table_`中删除目标槽位中的原页面ID的映射，并将新的<页面ID - 槽位ID>映射插入，然后更新槽位中页面的元数据。需要注意的是，在这里由于我们返回了指向该页面的指针，我们需要将该页面的用户数`pin_count_`置为1，并调用`replacer_`的`Pin`。

```C++
107 Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
108   // 1.     Search the page table for the requested page (P).
109   // 1.1    If P exists, pin it and return it immediately.
110   // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
111   //        Note that pages are always found from the free list first.
112   // 2.     If R is dirty, write it back to the disk.
113   // 3.     Delete R from the page table and insert P.
114   // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
115   frame_id_t frame_id;
116   latch_.lock();
117   if (page_table_.count(page_id) != 0U) {
118     frame_id = page_table_[page_id];
119     pages_[frame_id].pin_count_++;
120     replacer_->Pin(frame_id);
121     latch_.unlock();
122     return &pages_[frame_id];
123   }
124 
125   if (!free_list_.empty()) {
126     frame_id = free_list_.front();
127     free_list_.pop_front();
128     page_table_[page_id] = frame_id;
129     disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
130     pages_[frame_id].pin_count_ = 1;
131     pages_[frame_id].page_id_ = page_id;
132     replacer_->Pin(frame_id);
133     latch_.unlock();
134     return &pages_[frame_id];
135   }
136   if (!replacer_->Victim(&frame_id)) {
137     latch_.unlock();
138     return nullptr;
139   }
140   if (pages_[frame_id].IsDirty()) {
141     page_id_t flush_page_id = pages_[frame_id].page_id_;
142     pages_[frame_id].is_dirty_ = false;
143     disk_manager_->WritePage(flush_page_id, pages_[frame_id].GetData());
144   }
145   page_table_.erase(pages_[frame_id].page_id_);
146   page_table_[page_id] = frame_id;
147   pages_[frame_id].page_id_ = page_id;
148   disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
149   pages_[frame_id].pin_count_ = 1;
150   replacer_->Pin(frame_id);
151   latch_.unlock();
152   return &pages_[frame_id];
153 }
```

`FetchPgImp`的功能是获取对应页面ID的页面，并返回指向该页面的指针，其由以下步骤组成：

1. 首先，通过检查`page_table_`以检查缓冲池中是否已经缓冲该页面，如果已经缓冲该页面，则直接返回该页面，并将该页面的用户数`pin_count_`递增以及调用`replacer_`的`Pin`方法；
2. 如缓冲池中尚未缓冲该页面，则需寻找当前缓冲池中是否存在空闲槽位或存放页面可被驱逐的槽位（下文称其为目标槽位），该流程与`NewPgImp`中的对应流程相似，唯一不同的则是传入目标槽位的`page_id`为函数参数而非由`AllocatePage()`分配得到。

```C++
155 bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
156   // 0.   Make sure you call DeallocatePage!
157   // 1.   Search the page table for the requested page (P).
158   // 1.   If P does not exist, return true.
159   // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
160   // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
161   DeallocatePage(page_id);
162   latch_.lock();
163   if (page_table_.count(page_id) == 0U) {
164     latch_.unlock();
165     return true;
166   }
167   frame_id_t frame_id;
168   frame_id = page_table_[page_id];
169   if (pages_[frame_id].pin_count_ != 0) {
170     latch_.unlock();
171     return false;
172   }
173   if (pages_[frame_id].IsDirty()) {
174     page_id_t flush_page_id = pages_[frame_id].page_id_;
175     pages_[frame_id].is_dirty_ = false;
176     disk_manager_->WritePage(flush_page_id, pages_[frame_id].GetData());
177   }
178   page_table_.erase(page_id);
179   pages_[frame_id].page_id_ = INVALID_PAGE_ID;
180   free_list_.push_back(frame_id);
181   latch_.unlock();
182   return true;
183 }
```

`DeletePgImp`的功能为从缓冲池中删除对应页面ID的页面，并将其插入空闲链表`free_list_`，其由以下步骤组成：

1. 首先，检查该页面是否存在于缓冲区，如未存在则返回True。然后，检查该页面的用户数`pin_count_`是否为0，如非0则返回False。在这里，不难看出`DeletePgImp`的返回值代表的是该页面是否被用户使用，因此在该页面不在缓冲区时也返回True；
2. 检查该页面是否为脏，如是则将其写回并将脏位设置为False。然后，在`page_table_`中删除该页面的映射，并将该槽位中页面的`page_id`置为`INVALID_PAGE_ID`。最后，将槽位ID插入空闲链表即可。

```C++
185 bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
186   latch_.lock();
187   frame_id_t frame_id;
188   if (page_table_.count(page_id) != 0U) {
189     frame_id = page_table_[page_id];
190     pages_[frame_id].is_dirty_ |= is_dirty;
191     if (pages_[frame_id].pin_count_ <= 0) {
192       latch_.unlock();
193       return false;
194     }
195     // std::cout<<"Unpin : pin_count = "<<pages_[frame_id].pin_count_<<std::endl;
196     if (--pages_[frame_id].pin_count_ == 0) {
197       replacer_->Unpin(frame_id);
198     }
199   }
200   latch_.unlock();
201   return true;
202 }
```

`UnpinPgImp`的功能为提供用户向缓冲池通知页面使用完毕的接口，用户需声明使用完毕页面的页面ID以及使用过程中是否对该页面进行修改。其由以下步骤组成：

1. 首先，需检查该页面是否在缓冲池中，如未在缓冲池中则返回True。然后，检查该页面的用户数是否大于0，如不存在用户则返回false；
2. 递减该页面的用户数`pin_count_`，如在递减后该值等于0，则调用`replacer_->Unpin`以表示该页面可以被驱逐。

## Task 3 ：PARALLEL BUFFER POOL MANAGER

不难看出，上述缓冲池实现的问题在于锁的粒度过大，其在进行任何一项操作时都将整个缓冲池锁住，因此几乎不存在并行性。在这里，并行缓冲池的思想是分配多个独立的缓冲池，并将不同的页面ID映射至各自的缓冲池中，从而减少整体缓冲池的锁粒度，以增加并行性。

```C++
 25 class ParallelBufferPoolManager : public BufferPoolManager {
...
 93  private:
 94   std::vector<BufferPoolManager *> instances_;
 95   size_t start_idx_{0};
 96   size_t pool_size_;
 97   size_t num_instances_;
 98 };
```

并行缓冲池的成员如上，`instances_`用于存储多个独立的缓冲池，`pool_size_`记录各缓冲池的容量，`num_instances_`为独立缓冲池的个数，`start_idx`见下文介绍。

```C++
 18 ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, Disk    Manager *disk_manager,
 19                                                      LogManager *log_manager)
 20     : pool_size_(pool_size), num_instances_(num_instances) {
 21   // Allocate and create individual BufferPoolManagerInstances
 22   for (size_t i = 0; i < num_instances; i++) {
 23     BufferPoolManager *tmp = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_mana    ger, log_manager);
 24     instances_.push_back(tmp);
 25   }
 26 }
 27 
 28 // Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated me    mory
 29 ParallelBufferPoolManager::~ParallelBufferPoolManager() {
 30   for (size_t i = 0; i < num_instances_; i++) {
 31     delete (instances_[i]);
 32   }
 33 }
```

在这里，各独立缓冲池在堆区中进行分配，构造函数和析构函数需要完成相应的分配和释放工作。

```C++
 35 size_t ParallelBufferPoolManager::GetPoolSize() {
 36   // Get size of all BufferPoolManagerInstances
 37   return num_instances_ * pool_size_;
 38 }
 39 
 40 BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
 41   // Get BufferPoolManager responsible for handling given page id. You can use this method in you    r other methods.
 42   return instances_[page_id % num_instances_];
 43 }
```

需要注意的是，`GetPoolSize`应返回全部缓冲池的容量，即独立缓冲池个数乘以缓冲池容量。

`GetBufferPoolManager`返回页面ID所对应的独立缓冲池指针，在这里，通过对页面ID取余的方式将页面ID映射至对应的缓冲池。

```C++
 45 Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
 46   // Fetch page for page_id from responsible BufferPoolManagerInstance
 47   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 48   return instance->FetchPage(page_id);
 49 }
 50 
 51 bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
 52   // Unpin page_id from responsible BufferPoolManagerInstance
 53   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 54   return instance->UnpinPage(page_id, is_dirty);
 55 }
 56 
 57 bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
 58   // Flush page_id from responsible BufferPoolManagerInstance
 59   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 60   return instance->FlushPage(page_id);
 61 }
...
 82 bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
 83   // Delete page_id from responsible BufferPoolManagerInstance
 84   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 85   return instance->DeletePage(page_id);
 86 }
 87 
 88 void ParallelBufferPoolManager::FlushAllPgsImp() {
 89   // flush all pages from all BufferPoolManagerInstances
 90   for (size_t i = 0; i < num_instances_; i++) {
 91     instances_[i]->FlushAllPages();
 92   }
 93 }
```

上述函数仅需调用对应独立缓冲池的方法即可。值得注意的是，由于在缓冲池中存放的为缓冲池实现类的基类指针，因此所调用函数的应为缓冲池实现类的基类对应的虚函数。并且，由于`ParallelBufferPoolManager`和`BufferPoolManagerInstance`为兄弟关系，因此`ParallelBufferPoolManager`不能直接调用`BufferPoolManagerInstance`对应的`Imp`函数，因此直接在`ParallelBufferPoolManager`中存放`BufferPoolManagerInstance`指针也是不可行的。

```C++
 63 Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
 64   // create new page. We will request page allocation in a round robin manner from the underlying
 65   // BufferPoolManagerInstances
 66   // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return     2) looped around to
 67   // starting index and return nullptr
 68   // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
 69   // is called
 70   Page *ret;
 71   for (size_t i = 0; i < num_instances_; i++) {
 72     size_t idx = (start_idx_ + i) % num_instances_;
 73     if ((ret = instances_[idx]->NewPage(page_id)) != nullptr) {
 74       start_idx_ = (*page_id + 1) % num_instances_;
 75       return ret;
 76     }
 77   }
 78   start_idx_++;
 79   return nullptr;
 80 }
```

在这里，为了使得各独立缓冲池的负载均衡，采用轮转方法选取分配物理页面时使用的缓冲池，在这里具体的规则如下：

1. 从`start_idx_`开始遍历各独立缓冲池，如存在调用`NewPage`成功的页面，则返回该页面并将`start_idx`指向该页面的下一个页面；
2. 如全部缓冲池调用`NewPage`均失败，则返回空指针，并递增`start_idx`。