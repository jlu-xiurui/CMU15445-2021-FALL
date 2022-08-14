//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <cstdio>
#include <iostream>
#include "common/macros.h"
namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  frame_id_t frame_id;
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id = it->second;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  latch_.lock();
  for (auto [page_id, frame_id] : page_table_) {
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  latch_.unlock();
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t new_frame_id;
  latch_.lock();
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&new_frame_id)) {
    latch_.unlock();
    return nullptr;
  }
  *page_id = AllocatePage();
  if (pages_[new_frame_id].IsDirty()) {
    page_id_t flush_page_id = pages_[new_frame_id].page_id_;
    pages_[new_frame_id].is_dirty_ = false;
    disk_manager_->WritePage(flush_page_id, pages_[new_frame_id].GetData());
  }
  page_table_.erase(pages_[new_frame_id].page_id_);
  page_table_[*page_id] = new_frame_id;
  pages_[new_frame_id].page_id_ = *page_id;
  pages_[new_frame_id].ResetMemory();
  pages_[new_frame_id].pin_count_ = 1;
  replacer_->Pin(new_frame_id);
  latch_.unlock();
  return &pages_[new_frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_id;
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id = it->second;
    Page *ret = &pages_[frame_id];
    ret->pin_count_++;
    replacer_->Pin(frame_id);
    latch_.unlock();
    return ret;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = frame_id;
    Page *ret = &pages_[frame_id];
    disk_manager_->ReadPage(page_id, ret->data_);
    ret->pin_count_ = 1;
    ret->page_id_ = page_id;
    replacer_->Pin(frame_id);
    latch_.unlock();
    return ret;
  }
  if (!replacer_->Victim(&frame_id)) {
    latch_.unlock();
    return nullptr;
  }
  if (pages_[frame_id].IsDirty()) {
    page_id_t flush_page_id = pages_[frame_id].page_id_;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->WritePage(flush_page_id, pages_[frame_id].GetData());
  }
  page_table_.erase(pages_[frame_id].page_id_);
  page_table_[page_id] = frame_id;
  pages_[frame_id].page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].pin_count_ = 1;
  replacer_->Pin(frame_id);
  latch_.unlock();
  return &pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  DeallocatePage(page_id);
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = it->second;
  if (pages_[frame_id].pin_count_ != 0) {
    latch_.unlock();
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    page_id_t flush_page_id = pages_[frame_id].page_id_;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->WritePage(flush_page_id, pages_[frame_id].GetData());
  }
  page_table_.erase(it);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  frame_id_t frame_id;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id = it->second;
    pages_[frame_id].is_dirty_ |= is_dirty;
    if (pages_[frame_id].pin_count_ <= 0) {
      latch_.unlock();
      return false;
    }
    if (--pages_[frame_id].pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
  }
  latch_.unlock();
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
