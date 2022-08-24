# Project 2 : EXTENDIBLE HASH INDEX

在本实验中，需要实现一个**磁盘备份**的**可扩展哈希表**，用于DBMS中的索引检索。磁盘备份指该哈希表可写入至磁盘中，在系统重启时可以将其重新读取至内存中使用。可扩展哈希表是动态哈希表的一种类型，其特点为桶在充满或清空时可以桶为单位进行桶分裂或合并，尽在特定情况下进行哈希表全表的扩展和收缩，以减小扩展和收缩操作对全表的影响。

本文介绍了书中未讲解的**低位可拓展哈希表**的原理及其实现，且原理与实现之间设置了跳转以方便阅读。

## 可扩展哈希表实现原理

在进行实验之前，我们应当了解可扩展哈希表的具体实现原理。在这里，其最根本的思想在于通过改变哈希表用于映射至对应桶的哈希键位数来控制哈希表的总大小，该哈希键位数被称为全局深度。下面是全局深度的一个例子：

![figure 1](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%201.png)

上图为通过哈希函数对字符串产生哈希键的一个示例。可见，当哈希键的位数为32位时，不同的哈希键有2^32个，这代表哈希表将拥有上述数目的目录项以将哈希键映射至相应的哈希桶，该数目显然过于庞大了。因此，我们可以仅关注哈希键的低几位（高几位亦可，但使用低位更易实现）以缩小哈希表目录项的个数。例如，当我们仅关注哈希键的后三位时，不同的哈希键为`...000`至`...111`共8个，因此我们仅需为哈希表保存8个目录项即可将各低位不同的哈希键映射至对应的哈希表。

![figure 2](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%202.png)

除了用于控制哈希表大小的全局深度外，每个哈希表目录项均具有一个**局部深度**，其记录该目录项所对应的哈希桶所关注的哈希键位数。因此，局部深度**以桶为单位**划分的，某个目录项的局部深度即为该目录项所指的桶的局部深度。例如，如上图可示，当表的全局深度为3，第`001`个目录项的局部深度为2时，哈希键为`...01`的所有键均将被映射至该目录项所对应的哈希桶中，即`001`和`101`两个目录项。因此，当哈希表的全局深度为`i`，某目录项的局部深度为`j`时，指向该目录项所对应的哈希桶的目录项个数为`2^(i-j)`。

下面，我将使用一个例子来展示可扩展哈希表的桶分裂/合并，表扩展/收缩行为。在说明中，将使用`i`代表表的全局深度，`j`代表目录项的局部深度：

![figure 3](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%203.png)

<a name="splitmethod1"></a>如上图所示，当哈希表刚被创建时，其全局深度为0，即哈希表仅有一个目录项，任何一个哈希键都将被映射到同一个哈希桶。当该哈希桶被充满时，需要进行桶的分裂，在这里，桶分裂的方式有两种，其对应于桶对应目录项的局部深度小于全局深度、桶对应目录项的局部深度等于全局深度两种情况。

当桶对应目录项的局部深度等于全局深度时，指向该桶的目录项仅有一条，因此需要进行表拓展。表拓展后，将表的全局深度加一，并将指向原被分裂桶和新桶的两个目录项置为当前的全局深度，并将原哈希桶的记录重新插入至新哈希桶或原哈希桶。对于其他目录项，表扩展后低`i-1`位相同的目录项指向同一桶页面，低第`i`位相反的两个页面互为**分裂映像**（实验中的自命名词汇）。[代码实现见下文](#splitcode)。

注意，可能分裂后的记录仍然均指向同一哈希桶，在这种情况下需要继续扩展哈希表，为了方便讲解，在本章节中不考虑这种特殊情况。因此，当上图中的哈希桶充满时，哈希表将更新至下图所示形式：

![figure 4](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%204.png)

在这里，表的全局深度由0变为1、两个目录项的局部深度被置为当前全局深度1。下面，当`...0`目录项所对应的桶被充满时，由于全局深度和该目录项的局部深度仍然相同，因此仍需进行表扩展：

![figure 5](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%205.png)

下面，当`...00`目录项所对应的桶充满时，由于全局深度和该目录项的局部深度仍然相同，因此仍需进行表扩展：

![figure 6](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%206.png)

<a name="splitmethod2"></a>此时，当.`..001`目录项所对应的桶充满时，由于该目录项的局部深度`j`小于全局深度`i`，因此有`2^(i-j)`个目录项指向所需分裂的哈希桶，因此不必进行表的拓展，仅需将桶分裂，并将原哈希桶映射的目录项的一半指向原哈希桶，另一半指向新哈希桶，最后将指向原哈希桶和新哈希桶的所有目录项的局部深度加一即可。划分的规则为低`j+1`位相同的目录项在分裂后仍指向同一个桶，这种分裂规则保证了局部深度的语义，即分裂后桶关注哈希键的低`j+1`位。

另一个可能的问题是，如何找到与该目录项指向同一哈希桶的其他目录项。在这里，对于全局深度为`i`，局部深度为`j`的目录项，与其共同指向同一哈希桶的目录项（下面将其称为兄弟目录项）的低`j`位相同，且通过以下三个特性可以方便的遍历所有兄弟目录项：

- 兄弟目录项中的最顶端（位表示最小）目录项为低`j`位不变、其余位为0的目录项；
- 相邻两个目录项的哈希键相差`1<<j`；
- 兄弟目录项的总数为`1<<(i - j)`。

上述操作代码实现[见下文](#splitcode)，分裂后的哈希表如下所示：

![figure 7](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%207.png)

<a name="mergemethod"></a>可以看出低`j+1 = 2`位相同的目录项在分裂后指向同一哈希桶，即以`...01`和`...11`为结尾的目录项分别指向两个不同的哈希桶。当一个目录项所指的哈希桶为空时，需要判断其是否可以与其**目标目录项**所指的哈希桶合并。一个目录项的目标目录项可由其低第`j`位反转得到，值得注意的是，由于目录项间的局部深度可能不同，因此目标目录项不一定是可逆的。例如，上图中`...010`目录项的目标目录项为`...000`，而`...000`的目标目录项却为`...100`。目录项及其目标目录项所指的两个哈希桶的合并的条件如下：（1）两哈希桶均为空桶；（2）目录项及其目标目录项的局部深度相同且不为0。此时，若`...001`和`...011`目录项所指的两个哈希桶均为空，则可以进行合并（代码实现见[下文](#mergecode)）：

![figure 8](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%208.png)

合并后，需要将指向合并后哈希桶的所有目录项的局部深度减一。此时，若`...000`和`...100`所指的哈希桶均为空，则可以进行合并：

![figure 9](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%209.png)

当哈希桶合并后使得所有目录项的局部深度均小于全局深度时，既可以进行哈希表的收缩。在这里可以体现低位可拓展哈希表，即收缩哈希表仅需将全局深度减一即可，而不需改变其余任何哈希表的元数据。下图展示了哈希表收缩后的形态：

![figure 10](https://github.com/jlu-xiurui/CMU15445-2021-FALL/blob/ghess/p2-refinement/notes/project2_figure/figure%2010.png)

## Task 1 : PAGE LAYOUTS

为了能在磁盘中写入和读取该哈希表，在这里需要实现两个页面类存储哈希表的数据，其使用上实验中的`Page`页面作为载体，以在磁盘中被写入和读取，具体的实现原理将在下文中介绍：

### HashTableDirectoryPage

```C++
 25 /**
 26  *
 27  * Directory Page for extendible hash table.
 28  *
 29  * Directory format (size in byte):
 30  * --------------------------------------------------------------------------------------------
 31  * | LSN (4) | PageId(4) | GlobalDepth(4) | LocalDepths(512) | BucketPageIds(2048) | Free(1524)
 32  * --------------------------------------------------------------------------------------------
 33  */
 34 class HashTableDirectoryPage {
 35  public:
 ...
189  private:
190   page_id_t page_id_;
191   lsn_t lsn_;
192   uint32_t global_depth_{0};
193   uint8_t local_depths_[DIRECTORY_ARRAY_SIZE];
194   page_id_t bucket_page_ids_[DIRECTORY_ARRAY_SIZE];
195 };

```

该页面类作为哈希表的目录页面，保存哈希表中使用的所有元数据，包括该页面的页面ID，日志序列号以及哈希表的全局深度、局部深度及各目录项所指向的桶的页面ID。在本实验中，`GetSplitImageIndex`和`GetLocalHighBit`两个与分裂映像相关的概念并未用到，个人认为此概念并不关键。下面将展示一些稍有难度的函数实现：

```C++
 29 uint32_t HashTableDirectoryPage::GetGlobalDepthMask() { return (1U << global_depth_) - 1; }
...
 47 bool HashTableDirectoryPage::CanShrink() {
 48   uint32_t bucket_num = 1 << global_depth_;
 49   for (uint32_t i = 0; i < bucket_num; i++) {
 50     if (local_depths_[i] == global_depth_) {
 51       return false;
 52     }
 53   }
 54   return true;
 55 }
```

`GetGlobalDepthMask`通过位运算返回用于计算全局深度低位的掩码；`CanShrink()`检查当前所有有效目录项的局部深度是否均小于全局深度，以判断是否可以进行表合并。

### HashTableBucketPage

```C++
 37 template <typename KeyType, typename ValueType, typename KeyComparator>
 38 class HashTableBucketPage {
 39  public:
...
141  private:
142   // For more on BUCKET_ARRAY_SIZE see storage/page/hash_table_page_defs.h
143   char occupied_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
144   // 0 if tombstone/brand new (never occupied), 1 otherwise.
145   char readable_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
146   // Do not add any members below array_, as they will overlap.
147   MappingType array_[0];
```

该页面类用于存放哈希桶的键值与存储值对，以及桶的槽位状态数据。`occupied_`数组用于统计桶中的槽是否被使用过，当一个槽被插入键值对时，其对应的位被置为1，事实上，`occupied_`完全可以被一个`size`参数替代，但由于测试用例中需要检测对应的`occupied`值，因此在这里仍保留该数组；`readable_`数组用于标记桶中的槽是否被占用，当被占用时该值被置为1，否则置为0；`array_`是C++中一种弹性数组的写法，在这里只需知道它用于存储实际的键值对即可。

在这里，使用`char`类型存放两个状态数据数组，在实际使用应当按位提取对应的状态位。下面是使用位运算的状态数组读取和设置函数：

```C++
 87 template <typename KeyType, typename ValueType, typename KeyComparator>
 88 void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
 89   readable_[bucket_idx / 8] &= ~(1 << (7 - (bucket_idx % 8)));
 90 }
 91 
 92 template <typename KeyType, typename ValueType, typename KeyComparator>
 93 bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
 94   return (occupied_[bucket_idx / 8] & (1 << (7 - (bucket_idx % 8)))) != 0;
 95 }
 96 
 97 template <typename KeyType, typename ValueType, typename KeyComparator>
 98 void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
 99   occupied_[bucket_idx / 8] |= 1 << (7 - (bucket_idx % 8));
100 }
101 
102 template <typename KeyType, typename ValueType, typename KeyComparator>
103 bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
104   return (readable_[bucket_idx / 8] & (1 << (7 - (bucket_idx % 8)))) != 0;
105 }
106 
107 template <typename KeyType, typename ValueType, typename KeyComparator>
108 void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
109   readable_[bucket_idx / 8] |= 1 << (7 - (bucket_idx % 8));
110 }
```

对于对应索引的键值读取直接访问`arrat_`数组即可：

```C++
 77 template <typename KeyType, typename ValueType, typename KeyComparator>
 78 KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
 79   return array_[bucket_idx].first;
 80 }
 81 
 82 template <typename KeyType, typename ValueType, typename KeyComparator>
 83 ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
 84   return array_[bucket_idx].second;
 85 }
```

```C++
 22 template <typename KeyType, typename ValueType, typename KeyComparator>
 23 bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
 24   bool ret = false;
 25   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
 26     if (!IsOccupied(bucket_idx)) {
 27       break;
 28     }
 29     if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0) {
 30       result->push_back(array_[bucket_idx].second);
 31       ret = true;
 32     }
 33   }
 34   return ret;
 35 }

```

`GetValue`提取桶中键为`key`的所有值，实现方法为遍历所有`occupied_`为1的位，并将键匹配的值插入`result`数组即可，如至少找到了一个对应值，则返回真。在这里，可以看出

```C++
 37 template <typename KeyType, typename ValueType, typename KeyComparator>
 38 bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
 39   size_t slot_idx = 0;
 40   bool slot_found = false;
 41   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
 42     if (!slot_found && (!IsReadable(bucket_idx) || !IsOccupied(bucket_idx))) {
 43       slot_found = true;
 44       slot_idx = bucket_idx;
 45       // LOG_DEBUG("slot_idx = %ld", bucket_idx);
 46     }
 47     if (!IsOccupied(bucket_idx)) {
 48       break;
 49     }
 50     if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx    )) {
 51       return false;
 52     }
 53   }
 54   if (slot_found) {
 55     SetReadable(slot_idx);
 56     SetOccupied(slot_idx);
 57     array_[slot_idx] = MappingType(key, value);
 58     return true;
 59   }
 60   return false;
 61 }
```

`Insert`向桶插入键值对，其先检测该键值对是否已经被插入到桶中，如是则返回假；如未找到该键值对，则从小到大遍历所有`occupied_`为1的位，如出现`readable_`为1的位，则在`array_`中对应的数组中插入键值对。由于此种插入特性，因此`occupied_`为1的位是连续的，因此`occupied_`的功能与一个`size`参数是等价的。在这里仍然采用`occupied_`数组的原因可能是提供静态哈希表的实现兼容性（静态哈希表采用线性探测法解决散列冲突，因此必须使用`occupied_`数组）。

```C++
 63 template <typename KeyType, typename ValueType, typename KeyComparator>
 64 bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
 65   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
 66     if (!IsOccupied(bucket_idx)) {
 67       break;
 68     }
 69     if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx    )) {
 70       RemoveAt(bucket_idx);
 71       return true;
 72     }
 73   }
 74   return false;
 75 }
```

`Remove`从桶中删除对应的键值对，遍历桶所有位即可。

```C++
112 template <typename KeyType, typename ValueType, typename KeyComparator>
113 bool HASH_TABLE_BUCKET_TYPE::IsFull() {
114   return NumReadable() == BUCKET_ARRAY_SIZE;
115 }
116 
117 template <typename KeyType, typename ValueType, typename KeyComparator>
118 uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
119   uint32_t ret = 0;
120   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
121     if (!IsOccupied(bucket_idx)) {
122       break;
123     }
124     if (IsReadable(bucket_idx)) {
125       ret++;
126     }
127   }
128   return ret;
129 } 
130     
131 template <typename KeyType, typename ValueType, typename KeyComparator>
132 bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
133   return NumReadable() == 0;
134 }
```

`NumReadable()`返回桶中的键值对个数，遍历即可。`IsFull()`和`IsEmpty()`直接复用`NumReadable()`实现。

### Page与上述两个页面类的转换

在本部分中，有难点且比较巧妙的地方在于理解上述两个页面类是如何与`Page`类型转换的。在这里，上述两个页面类并非未`Page`类的子类，在实际应用中通过`reinterpret_cast`将`Page`与两个页面类进行转换。在这里我们回顾一下`Page`的数据成员：

```C++
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

可以看出，`Page`中用于存放实际数据的`data_`数组位于数据成员的第一位，其在栈区固定分配一个页面的大小。因此，在`Page`与两个页面类强制转换时，通过两个页面类的指针的操作仅能影响到`data_`中的实际数据，而影响不到其它元数据。并且在内存管理器中始终是进行所占空间更大的通用页面`Page`的分配（实验中的`NewPage`），因此页面的容量总是足够的。

## Task 2,3 : HASH TABLE IMPLEMENTATION + CONCURRENCY CONTROL

在这两个部分中，我们需要实现一个线程安全的可扩展哈希表。在对可扩展哈希表的原理清楚后，将其实现并不困难，难点在于如何在降低锁粒度、提高并发性的情况下保证线程安全。下面是哈希表的具体实现：

```C++
 24 template <typename KeyType, typename ValueType, typename KeyComparator>
 25 HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
 26                                      const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
 27     : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
 28   // LOG_DEBUG("BUCKET_ARRAY_SIZE = %ld", BUCKET_ARRAY_SIZE);
 29   HashTableDirectoryPage *dir_page =
 30       reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
 31   dir_page->SetPageId(directory_page_id_);
 32   page_id_t new_bucket_id;
 33   buffer_pool_manager_->NewPage(&new_bucket_id);
 34   dir_page->SetBucketPageId(0, new_bucket_id);
 35   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
 36   assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
 37 }
```

在构造函数中，为哈希表分配一个目录页面和桶页面，并设置目录页面的`page_id`成员、将哈希表的首个目录项指向该桶。最后，不要忘记调用`UnpinPage`向缓冲池告知页面的使用完毕。

```C++
 54 template <typename KeyType, typename ValueType, typename KeyComparator>
 55 uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
 56   uint32_t hashed_key = Hash(key);
 57   uint32_t mask = dir_page->GetGlobalDepthMask();
 58   return mask & hashed_key;
 59 }
 60 
 61 template <typename KeyType, typename ValueType, typename KeyComparator>
 62 page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
 63   uint32_t idx = KeyToDirectoryIndex(key, dir_page);
 64   return dir_page->GetBucketPageId(idx);
 65 }
 66 
 67 template <typename KeyType, typename ValueType, typename KeyComparator>
 68 HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
 69   return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
 70 }
 71 
 72 template <typename KeyType, typename ValueType, typename KeyComparator>
 73 HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
 74   return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
 75 }
 76 
```

上面是一些用于提取目录页面、桶页面以及目录页面中的目录项的功能函数。

```C++
 80 template <typename KeyType, typename ValueType, typename KeyComparator>
 81 bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
 82   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
 83   table_latch_.RLock();
 84   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
 85   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
 86   Page *p = reinterpret_cast<Page *>(bucket);
 87   p->RLatch();
 88   bool ret = bucket->GetValue(key, comparator_, result);
 89   p->RUnlatch();
 90   table_latch_.RUnlock();
 91   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
 92   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
 93 
 94   return ret;
 95 }
```

`GetValue`从哈希表中读取与键匹配的所有值结果，其通过哈希表的读锁保护目录页面，并使用桶的读锁保护桶页面。具体的操作步骤为先读取目录页面，再通过目录页面和哈希键或许对应的桶页面，最后调用桶页面的`GetValue`获取值结果。在函数返回时注意要`UnpinPage`所获取的页面。加锁时应当保证锁的获取、释放全局顺序以避免死锁。

```C++
100 template <typename KeyType, typename ValueType, typename KeyComparator>
101 bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value    ) {
102   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
103   table_latch_.RLock();
104   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
105   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
106   Page *p = reinterpret_cast<Page *>(bucket);
107   p->WLatch();
108   if (bucket->IsFull()) {
109     p->WUnlatch();
110     table_latch_.RUnlock();
111     assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
112     assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
113     return SplitInsert(transaction, key, value);
114   }
115   bool ret = bucket->Insert(key, value, comparator_);
116   p->WUnlatch();
117   table_latch_.RUnlock();
118   // std::cout<<"find the unfull bucket"<<std::endl;
119   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
120   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
121   return ret;
122 }
```

`Insert`向哈希表插入键值对，这可能会导致桶的分裂和表的扩张，因此需要保证目录页面的读线程安全，一种比较简单的保证线程安全的方法为：在操作目录页面前对目录页面加读锁。但这种加锁方式使得`Insert`函数阻塞了整个哈希表，这严重影响了哈希表的并发性。可以注意到，表的扩张的发生频率并不高，对目录页面的操作属于读多写少的情况，因此可以使用乐观锁的方法优化并发性能，其在`Insert`被调用时仅保持读锁，只在需要桶分裂时重新获得读锁。

`Insert`函数的具体流程为：

1. 获取目录页面和桶页面，在加全局读锁和桶写锁后检查桶是否已满，如已满则释放锁，并调用`UnpinPage`释放页面，然后调用`SplitInsert`实现桶分裂和插入；
2. 如当前桶未满，则直接向该桶页面插入键值对，并释放锁和页面即可。

```C++
124 template <typename KeyType, typename ValueType, typename KeyComparator>
125 bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
126   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
127   table_latch_.WLock();
128   while (true) {
129     page_id_t bucket_page_id = KeyToPageId(key, dir_page);
130     uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
131     HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
132     if (bucket->IsFull()) {
133       uint32_t global_depth = dir_page->GetGlobalDepth();
134       uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
135       page_id_t new_bucket_id = 0;
136       HASH_TABLE_BUCKET_TYPE *new_bucket =
137           reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_id));
138       assert(new_bucket != nullptr);
```

由于`SplitInsert`比较复杂，这里进行分段讲解：

**124-138行**：首先，获取目录页面并加全局写锁，在添加全局写锁后，其他所有线程均被阻塞了，因此可以放心的操作数据成员。不难注意到，在`Insert`中释放读锁和`SplitInsert`中释放写锁间存在空隙，其他线程可能在该空隙中被调度，从而改变桶页面或目录页面数据。因此，在这里需要重新在目录页面中获取哈希键所对应的桶页面（可能与`Insert`中判断已满的页面不是同一页面），并检查对应的桶页面是否已满。如桶页面仍然是满的，则分配新桶和提取原桶页面的元数据。在由于桶分裂后仍所需插入的桶仍可能是满的，因此在这这里进行循环以解决该问题。

```C++
139       if (global_depth == local_depth) {
140         // if i == ij, extand the bucket dir, and split the bucket
141         uint32_t bucket_num = 1 << global_depth;
142         for (uint32_t i = 0; i < bucket_num; i++) {
143           dir_page->SetBucketPageId(i + bucket_num, dir_page->GetBucketPageId(i));
144           dir_page->SetLocalDepth(i + bucket_num, dir_page->GetLocalDepth(i));
145         } 
146         dir_page->IncrGlobalDepth();
147         dir_page->SetBucketPageId(bucket_idx + bucket_num, new_bucket_id);
148         dir_page->IncrLocalDepth(bucket_idx);
149         dir_page->IncrLocalDepth(bucket_idx + bucket_num);
150         global_depth++;
151       } else {
152         // if i > ij, split the bucket
153         // more than one records point to the bucket
154         // the records' low ij bits are same
155         // and the high (i - ij) bits are index of the records point to the same bucket
156         uint32_t mask = (1 << local_depth) - 1;
157         uint32_t base_idx = mask & bucket_idx;
158         uint32_t records_num = 1 << (global_depth - local_depth - 1);
159         uint32_t step = (1 << local_depth);
160         uint32_t idx = base_idx;
161         for (uint32_t i = 0; i < records_num; i++) {
162           dir_page->IncrLocalDepth(idx);
163           idx += step * 2;
164         } 
165         idx = base_idx + step;
166         for (uint32_t i = 0; i < records_num; i++) {
167           dir_page->SetBucketPageId(idx, new_bucket_id);
168           dir_page->IncrLocalDepth(idx); 
169           idx += step * 2;
170         }
171       }
```

**139-171行**：在这里，需要根据全局深度和桶页面的局部深度判断扩展表和分裂桶的策略。当`global_depth == local_depth`时，需要进行表扩展和桶分裂，`global_depth == local_depth`仅需进行桶分裂即可。原理介绍见上文所示：[表扩展及分裂桶](#splitmethod1)、[仅分裂桶](#splitmethod2)，在这里不再赘述。<a name="splitcode"></a>

```C++
173       // rehash all records in bucket j
174       for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
175         KeyType j_key = bucket->KeyAt(i);
176         ValueType j_value = bucket->ValueAt(i);
177         bucket->RemoveAt(i);
178         if (KeyToPageId(j_key, dir_page) == bucket_page_id) {
179           bucket->Insert(j_key, j_value, comparator_);
180         } else {
181           new_bucket->Insert(j_key, j_value, comparator_);
182         }
183       }
184       // std::cout<<"original bucket size = "<<bucket->NumReadable()<<std::endl;
185       // std::cout<<"new bucket size = "<<new_bucket->NumReadable()<<std::endl;
186       assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
187       assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
```

**173-187行**：在完成桶分裂后，应当将原桶页面中的记录重新插入哈希表，由于记录的低`i-1`位仅与原桶页面和新桶页面对应，因此记录插入的桶页面仅可能为原桶页面和新桶页面两个选择。在重新插入完记录后，释放新桶页面和原桶页面。

```C++
188     } else {
189       bool ret = bucket->Insert(key, value, comparator_);
190       table_latch_.WUnlock();
191       // std::cout<<"find the unfull bucket"<<std::endl;
192       assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
193       assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
194       return ret;
195     }
196   }
197 
198   return false;
199 }
```

**188-198行**：若当前键值对所插入的桶页面非空（被其他线程修改或桶分裂后结果），则直接插入键值对，并释放锁和页面，并将插入结果返回`Insert`。

```C++
204 template <typename KeyType, typename ValueType, typename KeyComparator>
205 bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
206   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
207   table_latch_.RLock();
208   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
209   uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
210   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
211   Page *p = reinterpret_cast<Page *>(bucket);
212   p->WLatch();
213   bool ret = bucket->Remove(key, value, comparator_);
214   p->WUnlatch();
215   if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {
216     table_latch_.RUnlock();
217     this->Merge(transaction, key, value);
218   } else {
219     table_latch_.RUnlock();
220   }
221   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
222   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
223   return ret;
224 }
```

`Remove`从哈希表中删除对应的键值对，其优化思想与`Insert`相同，由于桶的合并并不频繁，因此在删除键值对时仅获取全局读锁，只在需要合并桶时获取全局写锁。当删除后桶为空且目录项的局部深度不为零时，释放读锁并调用`Merge`尝试合并页面，随后释放锁和页面并返回。

```C++
229 template <typename KeyType, typename ValueType, typename KeyComparator>
230 void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
231   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
232   table_latch_.WLock();
233   uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
234   page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
235   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
236   if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {
237     uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
238     uint32_t global_depth = dir_page->GetGlobalDepth();
239     // How to find the bucket to Merge?
240     // Answer: After Merge, the records, which pointed to the Merged Bucket,
241     // have low (local_depth - 1) bits same
242     // therefore, reverse the low local_depth can get the idx point to the bucket to Merge
243     uint32_t merged_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));
244     page_id_t merged_page_id = dir_page->GetBucketPageId(merged_bucket_idx);
245     HASH_TABLE_BUCKET_TYPE *merged_bucket = FetchBucketPage(merged_page_id);
246     if (dir_page->GetLocalDepth(merged_bucket_idx) == local_depth && merged_bucket->IsEmpty()) {
247       local_depth--;
248       uint32_t mask = (1 << local_depth) - 1;
249       uint32_t idx = mask & bucket_idx;
250       uint32_t records_num = 1 << (global_depth - local_depth);
251       uint32_t step = (1 << local_depth);
252 
253       for (uint32_t i = 0; i < records_num; i++) {
254         dir_page->SetBucketPageId(idx, bucket_page_id);
255         dir_page->DecrLocalDepth(idx);
256         idx += step;
257       }
258       buffer_pool_manager_->DeletePage(merged_page_id);
259     }
260     if (dir_page->CanShrink()) {
261       dir_page->DecrGlobalDepth();
262     }
263     assert(buffer_pool_manager_->UnpinPage(merged_page_id, true, nullptr));
264   }
265   table_latch_.WUnlock();
266   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
267   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
268 }
```

在`Merge`函数获取写锁后，需要重新判断是否满足合并条件，以防止在释放锁的空隙时页面被更改，在合并被执行时，需要判断当前目录页面是否可以收缩，如可以搜索在这里仅需递减全局深度即可完成收缩，最后释放页面和写锁。具体的合并细节和策略见[上文](#mergemethod)。<a name="mergecode"></a>

## 实验结果

![figure11](C:\Users\xiurui\Desktop\计算机书单\CMU15445\notes\project2_figure\figure11.png)
