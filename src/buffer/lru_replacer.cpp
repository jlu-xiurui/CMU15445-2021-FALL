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
#include <iostream>
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() {
  LinkListNode *p = head_;
  while (p != nullptr) {
    LinkListNode *tmp = p;
    p = p->next_;
    delete (tmp);
  }
}

void LRUReplacer::DeleteNode(LinkListNode *curr) {
  if (curr == head_ && curr == tail_) {
    head_ = nullptr;
    tail_ = nullptr;
  } else if (curr == head_) {
    head_ = head_->next_;
    curr->next_->prev_ = curr->prev_;
  } else if (curr == tail_) {
    tail_ = tail_->prev_;
    curr->prev_->next_ = curr->next_;
  } else {
    curr->prev_->next_ = curr->next_;
    curr->next_->prev_ = curr->prev_;
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
  auto it = data_idx_.find(frame_id);
  if (it != data_idx_.end()) {
    DeleteNode(data_idx_[frame_id]);
    data_idx_.erase(it);
  }
  data_latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  data_latch_.lock();
  auto it = data_idx_.find(frame_id);
  if (it == data_idx_.end()) {
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
