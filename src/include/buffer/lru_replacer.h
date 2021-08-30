//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <cstring>
#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
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

 private:
  // TODO(student): implement me!

  struct frame_status {
    frame_id_t frame_id;
    bool ref;
    frame_status(frame_id_t f, bool i) : frame_id(f), ref(i) {}
  };

  std::list<frame_status> unpinned_frames;
  std::list<frame_status>::iterator cursor;
  std::list<frame_status>::iterator *iterlookup;
  std::list<frame_status>::iterator flag;
  // std::mutex unpinned_frames_lock;
  // std::mutex cursor_lock;
  std::mutex latch_;
  size_t unpinned_size;
};

}  // namespace bustub
