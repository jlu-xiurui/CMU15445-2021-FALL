//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LinkListNode {
 public:
  frame_id_t val_{0};
  LinkListNode *prev_{nullptr};
  LinkListNode *next_{nullptr};
  explicit LinkListNode(frame_id_t Val) : val_(Val) {}
};
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

  void DeleteNode(LinkListNode *curr);

 private:
  // TODO(student): implement me!
  std::unordered_map<frame_id_t, LinkListNode *> data_idx_;
  LinkListNode *head_{nullptr};
  LinkListNode *tail_{nullptr};
  std::mutex data_latch_;
};

}  // namespace bustub
