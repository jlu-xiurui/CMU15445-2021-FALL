//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() {
  LinkListNode *ptr = head_;
  while (ptr != nullptr) {
    LinkListNode *tmp = ptr;
    ptr = ptr->next_;
    delete (tmp);
  }
}

void LRUReplacer::DeleteNode(LinkListNode *curr) {
  if (curr->prev_ != nullptr) {
    curr->prev_->next_ = curr->next_;
  }
  if (curr->next_ != nullptr) {
    curr->next_->prev_ = curr->prev_;
  }
  if (curr == head_ && curr == tail_) {
    head_ = nullptr;
    tail_ = nullptr;
  } else if (curr == head_) {
    head_ = head_->next_;
  } else if (curr == tail_) {
    tail_ = tail_->prev_;
  }
  delete (curr);
}
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  data_latch_.lock();
  if (data_idx_.empty()) {
    data_latch_.unlock();
    return false;
  }
  *frame_id = head_->val_;
  data_idx_.erase(head_->val_);
  LinkListNode *tmp = head_;
  DeleteNode(tmp);
  data_latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  data_latch_.lock();
  if (data_idx_.count(frame_id) != 0U) {
    DeleteNode(data_idx_[frame_id]);
    data_idx_.erase(frame_id);
  }
  data_latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  data_latch_.lock();
  if (data_idx_.count(frame_id) == 0U) {
    LinkListNode *new_node = new LinkListNode(frame_id);
    if (data_idx_.empty()) {
      head_ = tail_ = new_node;
    } else {
      tail_->next_ = new_node;
      new_node->prev_ = tail_;
      tail_ = new_node;
    }
    data_idx_[frame_id] = tail_;
  }
  data_latch_.unlock();
}

size_t LRUReplacer::Size() {
  data_latch_.lock();
  size_t ret = data_idx_.size();
  data_latch_.unlock();
  return ret;
}

}  // namespace bustub
